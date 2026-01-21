from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs, Transition


class RcStateRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def insert_state(
        self,
        ticker: str,
        date: str,
        state: State,
        reasons: list[ReasonCode],
        attrs: StateAttrs,
        run_id: str,
    ) -> None:
        reasons_json = json.dumps([reason.value for reason in reasons])
        self._conn.execute(
            """
            INSERT INTO rc_state_daily (
                ticker,
                date,
                state,
                reasons_json,
                confidence,
                age,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                state=excluded.state,
                reasons_json=excluded.reasons_json,
                confidence=excluded.confidence,
                age=excluded.age,
                run_id=excluded.run_id
            """,
            (
                ticker,
                date,
                state.value,
                reasons_json,
                attrs.confidence,
                attrs.age,
                run_id,
            ),
        )

    def insert_transition(
        self,
        ticker: str,
        date: str,
        transition: Transition | None,
        run_id: str,
    ) -> None:
        if transition is None:
            return

        reasons_json = json.dumps([reason.value for reason in transition.reason_codes])
        self._conn.execute(
            """
            INSERT OR REPLACE INTO rc_transition (
                ticker,
                date,
                from_state,
                to_state,
                reasons_json,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                transition.from_state.value,
                transition.to_state.value,
                reasons_json,
                run_id,
            ),
        )
