"""Tests for state_attrs_json persistence in rc_state_daily."""

from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.infra.sqlite.repos.rc_state_repo import RcStateRepo


def test_state_attrs_json_persisted_when_column_exists() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_state_daily (
            ticker TEXT,
            date TEXT,
            state TEXT,
            reasons_json TEXT,
            confidence INTEGER,
            age INTEGER,
            state_attrs_json TEXT NOT NULL DEFAULT '{}',
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_transition (
            ticker TEXT,
            date TEXT,
            from_state TEXT,
            to_state TEXT,
            reasons_json TEXT,
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_signal_daily (
            ticker TEXT,
            date TEXT,
            signal_keys_json TEXT,
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        )
        """
    )

    repo = RcStateRepo(conn)
    repo.insert_state(
        ticker="AAA",
        date="2026-01-02",
        state=State.DOWNTREND_EARLY,
        reasons=[],
        attrs=StateAttrs(
            confidence=None,
            age=0,
            status=None,
            downtrend_origin="TREND",
            decline_profile="SLOW_DRIFT",
        ),
        run_id="run-1",
    )

    row = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-02"),
    ).fetchone()
    assert row is not None
    assert json.loads(row[0]) == {
        "downtrend_origin": "TREND",
        "decline_profile": "SLOW_DRIFT",
    }
