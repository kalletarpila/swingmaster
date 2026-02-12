"""Tests for rc state repo."""

from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import ReasonCode, State, reason_to_persisted
from swingmaster.core.domain.models import StateAttrs, Transition
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
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


def test_reason_overlap_persisted_with_policy_prefix() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    reasons = [ReasonCode.TREND_STARTED]
    repo.insert_state(
        ticker="TEST.HE",
        date="2025-01-11",
        state=State.NO_TRADE,
        reasons=reasons,
        attrs=StateAttrs(confidence=None, age=0, status=None),
        run_id="run-2",
    )

    expected = json.dumps(
        [reason_to_persisted(ReasonCode.TREND_STARTED)],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    stored_state = conn.execute(
        "SELECT reasons_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-11"),
    ).fetchone()
    assert stored_state is not None
    assert stored_state[0] == expected


def test_pass_to_no_trade_empty_reasons_persists_pass_completed() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    transition = Transition(
        from_state=State.PASS,
        to_state=State.NO_TRADE,
        reason_codes=[],
    )
    repo.insert_transition(
        ticker="TEST.HE",
        date="2025-01-12",
        transition=transition,
        run_id="run-3",
    )

    stored = conn.execute(
        "SELECT reasons_json FROM rc_transition WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-12"),
    ).fetchone()
    assert stored is not None
    assert stored[0] == json.dumps(
        ["POLICY:PASS_COMPLETED"],
        separators=(",", ":"),
        ensure_ascii=False,
    )


def test_pass_to_no_trade_with_reason_preserved() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    transition = Transition(
        from_state=State.PASS,
        to_state=State.NO_TRADE,
        reason_codes=[ReasonCode.INVALIDATED],
    )
    repo.insert_transition(
        ticker="TEST.HE",
        date="2025-01-13",
        transition=transition,
        run_id="run-4",
    )

    stored = conn.execute(
        "SELECT reasons_json FROM rc_transition WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-13"),
    ).fetchone()
    assert stored is not None
    assert stored[0] == json.dumps(
        ["POLICY:INVALIDATED"],
        separators=(",", ":"),
        ensure_ascii=False,
    )


def test_entry_window_to_pass_empty_reasons_persists_completed() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    transition = Transition(
        from_state=State.ENTRY_WINDOW,
        to_state=State.PASS,
        reason_codes=[],
    )
    repo.insert_transition(
        ticker="TEST.HE",
        date="2025-01-14",
        transition=transition,
        run_id="run-5",
    )

    stored = conn.execute(
        "SELECT reasons_json FROM rc_transition WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-14"),
    ).fetchone()
    assert stored is not None
    assert stored[0] == json.dumps(
        ["POLICY:ENTRY_WINDOW_COMPLETED"],
        separators=(",", ":"),
        ensure_ascii=False,
    )


def test_signal_keys_persisted_sorted_unique() -> None:
    conn = sqlite3.connect(":memory:")
    _create_tables(conn)
    repo = RcStateRepo(conn)

    signals = SignalSet(
        signals={
            SignalKey.DOW_TREND_UP: Signal(
                key=SignalKey.DOW_TREND_UP,
                value=True,
                confidence=None,
                source="dow",
            ),
            SignalKey.TREND_STARTED: Signal(
                key=SignalKey.TREND_STARTED,
                value=True,
                confidence=None,
                source="trend",
            ),
            SignalKey.DOW_LAST_HIGH_HH: Signal(
                key=SignalKey.DOW_LAST_HIGH_HH,
                value=True,
                confidence=None,
                source="dow",
            ),
        }
    )

    repo.insert_signals(
        ticker="TEST.HE",
        date="2025-01-12",
        signals=signals,
        run_id="run-3",
    )

    expected = json.dumps(
        sorted(
            {
                SignalKey.DOW_TREND_UP.value,
                SignalKey.TREND_STARTED.value,
                SignalKey.DOW_LAST_HIGH_HH.value,
            }
        ),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    stored = conn.execute(
        "SELECT signal_keys_json FROM rc_signal_daily WHERE ticker=? AND date=?",
        ("TEST.HE", "2025-01-12"),
    ).fetchone()
    assert stored is not None
    assert stored[0] == expected
