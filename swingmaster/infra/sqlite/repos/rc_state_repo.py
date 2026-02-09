"""SQLite repository for rc_state_daily and rc_transition persistence.

Responsibilities:
  - Insert/update state and transition rows deterministically.
Must not:
  - Modify policy or signal logic; persistence only.
"""

from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import ReasonCode, State, reason_to_persisted
from swingmaster.core.domain.models import StateAttrs, Transition
from swingmaster.core.signals.models import SignalSet


class RcStateRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def _normalize_reasons(self, reasons: list[ReasonCode]) -> list[ReasonCode]:
        if ReasonCode.ENTRY_CONDITIONS_MET in reasons:
            return [ReasonCode.ENTRY_CONDITIONS_MET]
        return reasons

    def insert_state(
        self,
        ticker: str,
        date: str,
        state: State,
        reasons: list[ReasonCode],
        attrs: StateAttrs,
        run_id: str,
    ) -> None:
        normalized_reasons = self._normalize_reasons(reasons)
        reasons_json = json.dumps(
            [reason_to_persisted(reason) for reason in normalized_reasons],
            separators=(",", ":"),
            ensure_ascii=False,
        )
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

    def insert_signals(
        self,
        ticker: str,
        date: str,
        signals: SignalSet,
        run_id: str,
    ) -> None:
        signal_keys = sorted({key.value for key in signals.signals})
        signal_keys_json = json.dumps(
            signal_keys,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            INSERT INTO rc_signal_daily (
                ticker,
                date,
                signal_keys_json,
                run_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                signal_keys_json=excluded.signal_keys_json,
                run_id=excluded.run_id
            """,
            (
                ticker,
                date,
                signal_keys_json,
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

        normalized_reasons = self._normalize_reasons(transition.reason_codes)
        reasons_json = json.dumps(
            [reason_to_persisted(reason) for reason in normalized_reasons],
            separators=(",", ":"),
            ensure_ascii=False,
        )
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
