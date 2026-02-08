from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs, Transition
from swingmaster.infra.sqlite.repos.rc_state_repo import RcStateRepo


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
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


def test_entry_conditions_met_is_exclusive_in_persistence() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    reasons = [
        ReasonCode.STABILIZATION_CONFIRMED,
        ReasonCode.ENTRY_CONDITIONS_MET,
        ReasonCode.CHURN_GUARD,
    ]
    repo.insert_state(
        ticker="TEST.HE",
        date="2025-01-10",
        state=State.STABILIZING,
        reasons=reasons,
        attrs=StateAttrs(confidence=None, age=0, status=None),
        run_id="run-1",
    )

    expected = json.dumps(
        [ReasonCode.ENTRY_CONDITIONS_MET.value],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    stored_state = conn.execute(
        "SELECT reasons_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-10"),
    ).fetchone()
    assert stored_state is not None
    assert stored_state[0] == expected

    transition = Transition(
        from_state=State.STABILIZING,
        to_state=State.ENTRY_WINDOW,
        reason_codes=reasons,
    )
    repo.insert_transition(
        ticker="TEST.HE",
        date="2025-01-10",
        transition=transition,
        run_id="run-1",
    )
    stored_transition = conn.execute(
        "SELECT reasons_json FROM rc_transition WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-10"),
    ).fetchone()
    assert stored_transition is not None
    assert stored_transition[0] == expected
